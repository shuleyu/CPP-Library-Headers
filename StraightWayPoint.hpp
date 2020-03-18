#ifndef ASU_STRAIGHTWAYPOINT
#define ASU_STRAIGHTWAYPOINT

#define RE 6371

#include<cmath>

#include<LL2TP.hpp>
#include<Sph2Cart.hpp>
#include<DotDist.hpp>
#include<Cart2Sph.hpp>
#include<TP2LL.hpp>

/**************************************************************
 * This C++ template returns the geographic location (waypiont,
 * in longitude, latitude and depth) if one start from (lon1, lat1, depth1),
 * walk along a straight line to (lon2, lat2, depth2).
 * for (dist) kilometers.
 *
 * Range of inputs:
 *
 *     lon  : -180 ~ 180.
 *     lat  : -90  ~ 90.
 *     depth: 0 ~ 6371.
 *
 * input(s):
 * const double &lon1    ----  longitude of start point
 * const double &lat1    ----  latitude of start point
 * const double &depth1  ----  depth of start point
 * const double &lon2    ----  longitude of end point
 * const double &lat2    ----  latitude of end point
 * const double &depth2  ----  depth of end point
 * const double &dist    ----  in kilometer.
 *
 * return(s):
 * vector<double> ans    ----  {plon, plat, pdepth}, location of the waypoint.
 *
 * Shule Yu
 * Mar 09 2020
 *
 * Key words: waypoint, straight line.
 *
**************************************************************/

std::vector<double> StraightWayPoint(const double &lon1, const double &lat1, const double &depth1,
                                     const double &lon2, const double &lat2, const double &depth2,
                                     const double &dist){

    // Step 1. Calculate the cartesian coordinats of the given points.
    auto tp1 = LL2TP(lon1, lat1), tp2 = LL2TP(lon2, lat2);
    auto p1 = Sph2Cart(RE - depth1, tp1.first, tp1.second), p2 = Sph2Cart(RE - depth2, tp2.first, tp2.second);
    double len = DotDist(p1[0], p1[1], p1[2], p2[0], p2[1], p2[2]);

    double ra = dist / len;
    auto p3 = Cart2Sph(p1[0] + (p2[0] - p1[0]) * ra, p1[1] + (p2[1] - p1[1]) * ra, p1[2] + (p2[2] - p1[2]) * ra);
    auto tp3 = TP2LL(p3[1], p3[2]);
    return {tp3.first, tp3.second, RE - p3[0]};
    
}

#endif
